# Open Data Contract Standard export

## Status

Proposed normative contract for deterministic, offline-validated Open Data
Contract Standard (ODCS) export. Implementation must not be described as
supported until every acceptance requirement in this specification is
executable.

## Purpose

streamt projects already declare versioned Kafka sources, model outputs, column
contracts, classifications, and primary keys. The ODCS exporter turns those
declared facts into a portable contract document without requiring runtime
access and without inventing business, lifecycle, ownership, or schema facts.

The export is metadata only. It does not read or mutate Kafka, Schema Registry,
Flink, Connect, Gateway, deployment ownership state, or reviewed plans.

## Pinned standard and schema

The first supported target is **ODCS v3.1.0**:

- Standard documentation:
  <https://bitol-io.github.io/open-data-contract-standard/v3.1.0/>
- Release:
  <https://github.com/bitol-io/open-data-contract-standard/releases/tag/v3.1.0>
- Release commit:
  [`b9d3ffc5aabe9e058afe4469cabe5a218fe9946d`](https://github.com/bitol-io/open-data-contract-standard/commit/b9d3ffc5aabe9e058afe4469cabe5a218fe9946d)
- Companion JSON Schema at that commit:
  <https://raw.githubusercontent.com/bitol-io/open-data-contract-standard/b9d3ffc5aabe9e058afe4469cabe5a218fe9946d/schema/odcs-json-schema-v3.1.0.json>
- Uncompressed schema size: `86441` bytes.
- Uncompressed schema SHA-256:
  `2cb7dd6fe43344d2233e0406438622681dc3ebadcf8f0d606a15b40c8f6752c0`.
- Upstream license: Apache License 2.0.

The upstream documentation states that the textual standard takes precedence
when it conflicts with the companion JSON Schema. streamt therefore validates
against the pinned official schema and separately enforces the textual
standard's requirements. In particular, the textual standard requires a
top-level `schema` array even though the pinned companion schema does not list
it among its top-level required fields.

The packaged artifact is a gzip-compressed, base64-encoded text resource named
`src/streamt/docs/schemas/odcs-3.1.0.json.gz.b64`. Loading it verifies the
decoded bytes against the pinned size and checksum before JSON parsing. Export
never fetches a schema or reference from the network.

## Document boundary

One invocation emits **one ODCS document for one parsed streamt project**. The
document's `schema` array contains the project's declared sources followed by
its declared model outputs in canonical order.

This is the initial product boundary because:

1. The streamt project is the existing versioned authoring and validation
   boundary.
2. ODCS natively represents multiple schema objects in one contract document.
3. A per-model or per-topic export would require independent contract IDs,
   lifecycle statuses, and versions that the current DSL does not contain.
4. A non-standard wrapper containing several ODCS documents would no longer be
   an ODCS document.

The exporter does not silently coalesce a source and model that resolve to the
same physical topic. It rejects that ambiguity. Per-resource export can be
designed later only with an explicit identity and lifecycle model.

Every schema object identifies its streamt role with this truthful ODCS custom
property:

```yaml
customProperties:
  - property: streamtResourceType
    value: source
```

The value is `source` or `model`. A model with an explicit `contract` also
includes `streamtContractEnforced` with the exact declared boolean value. No
other streamt-specific custom properties are emitted in the first release.

## CLI contract metadata

ODCS requires a contract identity, version, and status. streamt does not
currently declare all three, so the exporter must not manufacture them.

The command surface is:

```text
streamt docs odcs \
  --contract-id <ID> \
  --status <STATUS> \
  [--contract-version <VERSION>] \
  [--format yaml|json] \
  [--output-file <PATH>] \
  [--project-dir <PATH>] \
  [--env <ENVIRONMENT>]
```

Rules:

- `--contract-id` is semantically required, must contain at least one
  non-whitespace character, and is copied exactly to root `id`.
- `--status` is semantically required, must contain at least one non-whitespace
  character, and is copied exactly to root `status`. The exporter does not default to `draft` or
  `active`. It does not restrict the value to the examples in the ODCS schema,
  because ODCS itself permits other strings.
- Root `version` uses `--contract-version` when present, otherwise the exact
  `project.version`. It must contain at least one non-whitespace character.
  Missing both is an error. The exporter does not
  default to `0.0.0` or `1.0.0` and does not rewrite the version.
- Root `name` is the exact `project.name`.
- Root `apiVersion` is `v3.1.0` and `kind` is `DataContract`.
- `project.description` is not forced into ODCS `description.purpose`,
  `description.usage`, or `description.limitations`; those have narrower
  meanings.

The options are validated inside the command so global JSON mode receives a
normal streamt structured error rather than Click's unstructured missing-option
usage output. Invalid export input or output uses
`E505_ODCS_INVALID`, exits non-zero, and includes a safe JSON path or
streamt field location when one is available.

## Canonical root shape

The emitted root key order is:

```yaml
apiVersion: v3.1.0
kind: DataContract
id: <explicit contract ID>
name: <project name>
version: <explicit or project version>
status: <explicit status>
schema: []
```

Optional root sections are omitted rather than serialized as `null` or empty
placeholders.

## Source mapping

Every declared source becomes one ODCS schema object:

| ODCS field | streamt field | Rule |
| --- | --- | --- |
| `id` | generated | Deterministic UUIDv5 described below |
| `name` | `source.name` | Exact |
| `logicalType` | constant | `object` |
| `physicalName` | `source.topic` | Exact |
| `physicalType` | constant | `topic` |
| `description` | `source.description` | Exact when present |
| `tags` | `source.tags` | Exact order and values when non-empty |
| `properties` | `source.columns` | Declared order; omit when no columns exist |
| `customProperties` | constant provenance | `streamtResourceType: source` |

`SchemaRef.fields` are already promoted to `source.columns` by the strict
project model, so they follow the same mapping. Registry names, subjects,
versions, formats, definitions, cluster references, freshness, event-time
configuration, regions, owners, and lifecycle ownership are not mapped.

## Model mapping

Every declared model becomes one ODCS schema object. Its property source is
selected without compiling or inferring SQL:

1. When `model.contract` exists, use its columns, including an explicitly empty
   column list. The existence of a contract prevents fallback.
2. Otherwise use explicit `model.columns` when present.
3. Otherwise omit `properties` and report an incomplete-schema warning.

| ODCS field | streamt field | Rule |
| --- | --- | --- |
| `id` | generated | Deterministic UUIDv5 described below |
| `name` | `model.name` | Exact |
| `logicalType` | constant | `object` |
| `description` | `model.description` | Exact when present |
| `tags` | `model.tags` | Exact order and values when non-empty |
| `properties` | selected columns above | Declared order; omit when empty |
| `customProperties` | constant provenance | `streamtResourceType: model` |
| `customProperties[]` | `model.contract.enforced` | `streamtContractEnforced`, exact, only when a contract exists |

For Kafka-output materializations (`topic`, `virtual_topic`, and `flink`),
`physicalType` is `topic` and `physicalName` is the exact explicit
`model.topic.name`, or the established model-name topic default when no name is
declared. Sink models do not receive a guessed physical name or physical type;
connector-specific destination maps are not a normalized data-object identity.

`model.sql`, macros, parameters, `from` references, Kafka `key`, Flink and
connector tuning, access, group, region, owner, lifecycle ownership, masking,
and allow/deny policy are not mapped. In particular, a Kafka message key is not
promoted to an ODCS primary key.

## Property mapping

Every selected source, model, or model-contract column maps as follows:

| ODCS field | streamt field | Rule |
| --- | --- | --- |
| `id` | generated | Deterministic UUIDv5 described below |
| `name` | column `name` | Exact |
| `physicalType` | column `type` | Exact declared string when present |
| `logicalType` | column `type` | Only for the unambiguous mapping below |
| `description` | column `description` | Exact when present |
| `classification` | column classification | Exact enum value when present |
| `required` | `ColumnDefinition.required` | Exact boolean |
| `required` | inverse of `ContractColumn.nullable` | Only when `nullable` is not `null` |
| `primaryKey` | membership in `model.primary_key` | `true` only for declared members |
| `primaryKeyPosition` | index in `model.primary_key` | One-based, only for declared members |

An exact entry in `model.security.classification` may supply classification for
the matching emitted model property. Two different explicit classifications
for the same property are an error; the exporter does not select one by
precedence.

The first release maps only these case-insensitive Flink SQL type families:

| Flink physical type family | ODCS `logicalType` |
| --- | --- |
| `CHAR`, `VARCHAR`, `STRING` | `string` |
| `TINYINT`, `SMALLINT`, `INT`, `INTEGER`, `BIGINT` | `integer` |
| `DECIMAL`, `NUMERIC`, `REAL`, `FLOAT`, `DOUBLE` | `number` |
| `BOOLEAN`, `BOOL` | `boolean` |
| `DATE` | `date` |
| `TIME` | `time` |
| `TIMESTAMP`, `TIMESTAMP_LTZ` | `timestamp` |

Parameters such as precision and scale remain in `physicalType`. Unknown,
binary, interval, `ROW`, `MAP`, and incompletely parsed `ARRAY` types retain
their exact physical type but omit `logicalType`. The exporter never falls back
to `string`, never creates array items, and never derives a type from SQL.

## Deterministic identifiers and ordering

The root contract ID is always supplied by the caller. It is not derived from
`project.name`: same-named repositories would collide, and a project rename
would silently change contract identity.

Schema-object and property IDs are canonical UUIDv5 strings. The namespace is
Python's standard `uuid.NAMESPACE_URL`. The UUID name is the UTF-8 JSON encoding
of one of these arrays using `ensure_ascii=False` and compact separators
`(',', ':')`:

```text
["streamt-odcs-v1", contract_id, resource_kind, resource_name]
["streamt-odcs-v1", contract_id, resource_kind, resource_name, column_name]
```

`resource_kind` is exactly `source` or `model`. There is no slugging,
case-folding, truncation, path normalization, or environment-dependent input.
The algorithm version is part of the seed and cannot change silently after
release.

Canonical document ordering is:

1. Sources sorted by exact name.
2. Models sorted by exact name.
3. Properties in declared order.
4. Tags in declared order.
5. Custom properties in the fixed order defined above.

The exporter rejects:

- duplicate source names, duplicate model names, or a source/model name
  collision;
- duplicate property names within an object;
- two distinct objects resolving to the same non-empty physical topic;
- a generated ID collision;
- a declared primary-key column absent from the selected model properties;
- conflicting explicit classifications;
- an empty project with no source or model schema objects.

It never resolves a collision by overwriting, suffixing, hashing only one side,
or merging declarations.

## Fields deliberately omitted

The following data is omitted because streamt either lacks the required ODCS
semantics or the scopes do not match:

- Root `description`, `domain`, `tenant`, deprecated `dataProduct`, support,
  price, team, roles, servers, SLA properties, authoritative definitions, and
  contract creation timestamp.
- Runtime endpoints, credentials, connection maps, schema-registry subjects,
  deployment environments, and backend observations.
- Source/model owners and lifecycle ownership; a string owner is not an ODCS
  team member or role definition.
- Exposure owners, consumer groups, contracts, access, freshness, and SLAs;
  exposure-specific consumer objectives are not project-wide contract SLAs.
- Data tests and failure actions. Sample scopes, windows, tolerances,
  throughput, foreign-key behavior, and failure actions do not all have a
  lossless ODCS quality mapping.
- SQL transformation logic and inferred column lineage.
- Processing-time and event-time attributes, partition counts, replication
  factors, Kafka configuration, and Flink state/runtime settings.

No omitted field is emitted as `null`, a fabricated default, or a vendor
extension merely to retain every streamt value.

## Offline validation

Generation and validation are one operation. The command must not serialize or
write a document that has not passed all stages:

1. Strictly parse the streamt project and requested environment using existing
   offline parser behavior.
2. Validate explicit contract ID, status, and version inputs.
3. Build the document from a field allowlist; do not serialize whole Pydantic
   models.
4. Enforce the semantic and collision rules in this specification.
5. Load, decode, decompress, size-check, checksum-check, and parse the bundled
   official ODCS schema.
6. Call `Draft201909Validator.check_schema` on the bundled schema.
7. Validate the document with `Draft201909Validator` and `FormatChecker`.
8. Enforce textual-standard requirements not enforced by the companion schema,
   including a present top-level `schema` array. Separately enforce streamt's
   project boundary that the array is non-empty.
9. Sort failures by JSON Pointer and message before rendering a single
   structured export error.

The bundled schema contains only local `$defs`. Tests must prove that validation
does not call HTTP clients, DNS, sockets, runtime deployer factories, or the
deployment-state factory.

## Serialization and output

`--format yaml` is the default. YAML uses safe-dump semantics with a
`SafeDumper` subclass that disables aliases, Unicode enabled, canonical
insertion order, and one trailing newline. JSON uses UTF-8, two-space
indentation, canonical insertion order, and one trailing newline. Neither
format adds timestamps or generated comments.

Without `--output-file`, text mode writes only the raw ODCS document to stdout.
Parser notices, incomplete-schema warnings, and diagnostics go to stderr so
stdout remains pipe-safe.

With `--output-file`, streamt validates and serializes first, then atomically
replaces the explicitly requested file. Text mode prints a short confirmation;
quiet mode prints nothing. A failed write must not leave a partial file.

Global `streamt --output json` retains the normal streamt command envelope. The
local `--format` affects raw text or file serialization, not the nested JSON
value. On success the envelope contains:

```json
{
  "status": "ok",
  "command": "docs odcs",
  "data": {
    "standard": "odcs",
    "standard_version": "3.1.0",
    "document": {},
    "serialization": "yaml",
    "output_file": null
  },
  "errors": [],
  "warnings": []
}
```

The complete document remains in `data.document` even when an output file is
requested. Warnings use the normal warnings array and stderr; errors use the
normal errors array and a non-zero exit. No raw document is printed before or
after the envelope.

## Security and determinism

The exporter reads only explicitly mapped declarative metadata. Tests search
all text, JSON envelope, warnings, and errors for runtime endpoints, usernames,
passwords, tokens, secret environment values, SQL text, and provider exception
content.

Given identical project bytes, environment inputs, contract ID, status,
contract version, format, and streamt version, the document bytes are
identical. A different filesystem path, checkout, current time, machine,
network state, runtime availability, or declaration ordering between otherwise
identical source/model sets does not change IDs or canonical object ordering.

## Non-goals

- One ODCS document per model, source, topic, schema subject, or environment.
- ODCS import, round-trip editing, or synchronization.
- Runtime or deployment integration.
- Catalog publication or registration.
- Data-quality, exposure, SLA, team, role, support, price, or server export.
- SQL compilation, type inference, lineage inference, or live schema
  enrichment.
- Inferring contract lifecycle from ownership, deployment, or environment.
- Adding ODCS-specific fields to the streamt project DSL in this slice.
- Supporting mutable `latest` schemas, multiple ODCS versions, or ODCS 4.
- Treating the historical `docs openapi` compatibility name as an ODCS alias.

## Acceptance requirements

1. The bundled bytes match the pinned commit, size, and SHA-256 and load from an
   installed wheel without network access.
2. Every emitted document uses `apiVersion: v3.1.0`, `kind: DataContract`, and
   passes the pinned `Draft201909Validator` plus local semantic checks.
3. Missing contract ID, status, or version fails before serialization with
   `E505_ODCS_INVALID` in global JSON mode.
4. One project emits one document containing every source and model exactly
   once in canonical order.
5. Contract columns take precedence whenever a model contract exists. SQL is
   never compiled or inspected to fill missing properties.
6. Unknown types preserve `physicalType` and omit `logicalType`; no unknown type
   becomes `string`.
7. UUIDs are stable across runs and checkouts, change with the explicit contract
   identity, and do not change when source/model declarations are reordered.
8. Duplicate logical names, property names, physical topics, generated IDs,
   missing primary-key properties, and classification conflicts fail closed.
9. Sink destinations, runtime configuration, secrets, owners, tests,
   exposures, SQL, and timestamps are absent from raw and structured output.
10. YAML and JSON stdout are parseable without removing warnings or Rich
    markup; all warnings are on stderr and in the structured warnings array.
11. Global JSON output contains exactly one normal streamt envelope with the
    document as structured data.
12. Output-file failure leaves no partial target, and successful output is the
    same validated serialization that stdout would have emitted.
