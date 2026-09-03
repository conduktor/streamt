# DataHub catalog export

Status: proposed. No DataHub export or publication support may be claimed until
the release gates in this specification pass.

This specification defines a deterministic, offline export of one compiled
streamt project as a JSON array of simplified DataHub Metadata Change
Proposals. Here, **DataHub MCP** always means a DataHub Metadata Change
Proposal.

## Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative. If a required identity,
relationship, or field cannot be represented exactly, the command MUST fail.
It must not normalize, merge, omit, or invent a replacement merely to produce
output.

## Pinned contract and release oracle

The first target is official DataHub `v1.7.0` at immutable commit
[`7f81ccbfe27b9acc947f5f600fcf9ddb72138a80`](https://github.com/datahub-project/datahub/commit/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80).

Production streamt has no DataHub runtime or optional dependency.
`acryl-datahub` MUST NOT appear in base requirements, a `streamt[datahub]`
extra, a runtime import, or a shipped wheel. Exact `acryl-datahub==1.7.0` is
installed only into isolated release-oracle environments. Broader DataHub
extras are not installed.

Normative upstream evidence is:

- [the v1.7.0 release](https://github.com/datahub-project/datahub/releases/tag/v1.7.0),
  which pins the CLI and Python SDK to `1.7.0`;
- [the official DataHub MCP file format](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/docs/advanced/writing-mcps.md),
  [MCP wrapper](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-ingestion/src/datahub/emitter/mcp.py),
  [metadata file writer](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-ingestion/src/datahub/ingestion/sink/file.py),
  and [metadata file reader](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-ingestion/src/datahub/ingestion/source/file.py);
- the pinned PDL for
  [`MetadataChangeProposal`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-models/src/main/pegasus/com/linkedin/mxe/MetadataChangeProposal.pdl),
  [`DataFlowInfo`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-models/src/main/pegasus/com/linkedin/datajob/DataFlowInfo.pdl),
  [`DataJobInfo`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-models/src/main/pegasus/com/linkedin/datajob/DataJobInfo.pdl),
  [`DataJobInputOutput`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-models/src/main/pegasus/com/linkedin/datajob/DataJobInputOutput.pdl),
  [`DatasetProperties`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-models/src/main/pegasus/com/linkedin/dataset/DatasetProperties.pdl), and
  [`DataPlatformInstance`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-models/src/main/pegasus/com/linkedin/common/DataPlatformInstance.pdl); and
- the pinned [`FabricType`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/li-utils/src/main/pegasus/com/linkedin/common/FabricType.pdl),
  [`DataPlatformUrn`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/li-utils/src/main/pegasus/com/linkedin/common/DataPlatformUrn.pdl),
  [`DataPlatformInstanceUrn`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/li-utils/src/main/pegasus/com/linkedin/common/DataPlatformInstanceUrn.pdl),
  [`DatasetUrn`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/li-utils/src/main/pegasus/com/linkedin/common/DatasetUrn.pdl),
  [`DataFlowUrn`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/li-utils/src/main/pegasus/com/linkedin/common/DataFlowUrn.pdl),
  and [`DataJobUrn`](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/li-utils/src/main/pegasus/com/linkedin/common/DataJobUrn.pdl).

Newer SDK or server behavior is not evidence for this contract. A version
change requires separate compatibility review.

## Scope and non-claims

The command emits an offline artifact. It does not:

- accept a DataHub URL, token, emitter, profile, or recipe;
- call DataHub, Kafka, Gateway, DNS, HTTP, sockets, or subprocesses;
- publish, patch, delete, soft-delete, read, reconcile, or roll back an entity;
- read deployment state, reviewed plans, operations, provider state, or
  recovery evidence;
- infer deletion from manifest absence or emit `DELETE`, `PATCH`, or `CREATE`;
- prove an entity, platform, or instance exists in a DataHub server;
- claim deployed Flink, Gateway, or Connect runtime telemetry; or
- import DataHub metadata into streamt.

Every emitted proposal has `changeType` exactly `UPSERT`. That is a reviewable
artifact, not authorization to ingest it.

Ownership, GlobalTags, native DataContracts, assertions, domains, glossary
terms, containers, schema/columns, fine-grained lineage, exposures, and live
publication are deferred. The adapter creates no placeholders and emits no
`status` or `subTypes` aspect.

## Command contract

```text
streamt docs datahub \
  --catalog-id <STABLE-ID> \
  --fabric <FABRIC-TYPE> \
  [--kafka-platform-instance <INSTANCE-ID>] \
  [--gateway-platform-id <PLATFORM-ID>] \
  [--gateway-platform-instance <INSTANCE-ID>] \
  [--output-file <PATH>] \
  [--project-dir <PATH>] \
  [--env <ENVIRONMENT>]
```

`--catalog-id` and `--fabric` are semantically required. They MUST validate
before project parsing, compilation, or file creation.

`--catalog-id` is the exact caller-owned DataFlow flow ID for this
project/environment. It must be non-empty, include non-whitespace, contain no
control or surrogate, and occupy at most 200 UTF-16 code units after the pinned
URN encoding. It is never inferred from project name, path, host, state, or
environment.

`--fabric` MUST be one exact uppercase v1.7.0 member:

```text
DEV TEST QA UAT EI PRE STG NON_PROD PROD CORP RVW PRD TST SIT SBX SANDBOX CERT
```

The selected streamt environment never implies a DataHub fabric. Changing
fabric changes every emitted entity URN: DataFlow, Dataset, and DataJob.

The command uses standard `--project-dir`, environment selection, `--quiet`,
and global output behavior. It runs `Compiler.compile(dry_run=True)` exactly
once and builds one neutral `CatalogSnapshot`. It constructs no state,
deployer, provider, DataHub, or network client.

### Transport identities

The first slice supports at most one platform instance per transport.

For ordinary Kafka datasets:

- platform is fixed to official `kafka`;
- `--kafka-platform-instance` is optional and is a bare instance ID;
- with it, the official platform-instance Dataset convention is used and a
  `dataPlatformInstance` aspect is emitted; and
- without it, the official no-instance Dataset convention is used and that
  aspect is omitted.

This option must match the official Kafka ingestion source's
`platform_instance` for assets to converge. Supplying an instance when that
source does not, or omitting one when it does, creates different URNs. Offline
streamt cannot verify that configuration.

For Gateway virtual-topic datasets, both `--gateway-platform-id` and
`--gateway-platform-instance` are conditionally required. DataHub v1.7.0 does
not bootstrap an official Conduktor Gateway platform, so streamt never invents
either value or creates the platform. An operator MAY deliberately use `kafka`
when an official Kafka source observes the Gateway proxy and the instance
matches exactly; streamt never chooses that mapping automatically.

Values are exact, non-blank bare IDs with no Unicode control or surrogate code
point. They are not URNs and are never trimmed, case-folded, slugged, or
hashed. Superfluous valid transport options MAY be accepted without output
effect.

### Pinned URN encoding and limits

The dependency-free renderer mirrors the v1.7.0 `UrnEncoder` exactly. It
replaces only these four characters:

| Raw character | Encoded text |
| --- | --- |
| comma `,` | `%2C` |
| left parenthesis `(` | `%28` |
| right parenthesis `)` | `%29` |
| U+241F `␟` | `%E2%90%9F` |

U+001F is a control character and is rejected. Slash, colon, dot, space,
literal percent, case, and Unicode normalization are otherwise preserved; the
renderer MUST NOT apply generic URL quoting or decode existing percent text.

All v1.7.0 PDL maxima below are enforced locally even where the generated
Python constructor does not enforce them. Length is Java string length: UTF-16
code units after component encoding and after final URN construction.

| Value | Maximum UTF-16 code units |
| --- | ---: |
| DataPlatform name / complete URN | 25 / 45 |
| complete DataPlatformInstance URN | 100 |
| composed Dataset name / complete URN | 210 / 284 |
| DataFlow orchestrator / flow ID / cluster / complete URN | 50 / 200 / 100 / 373 |
| DataJob job ID / complete URN | 200 / 594 |

The composed Dataset name is the encoded physical name without an instance,
or the encoded `instance + "." + physical_name` when an instance exists.
Successful local construction means the complete URN is byte-identical to the
pinned official constructor and official parse-then-stringify preserves the
constructed URN. It does not mean an encoded component equals its raw input.

The official encoder is intentionally non-injective because literal percent
text is not escaped: raw `a,b` and raw `a%2Cb` both encode as `a%2Cb`.
streamt preserves this behavior for Kafka-source interoperability. It rejects
collisions among all identities in one export, but cannot detect the same
alias across separate exports. Changing a raw identity component is therefore
not guaranteed to change its URN when those two official aliases are involved.

## Neutral projection boundary

The sole semantic input is the private immutable projection in
`streamt.integrations.catalog.model`; DataHub types MUST NOT enter it. The
adapter may read only project name/description, dataset logical and physical
identity, transport, descriptions, owner labels, tags, contract state, process
identity/kind/descriptions, exact direct dependencies, and exposure names for
warnings.

It MUST NOT inspect manifests, raw compiler objects, runtime config, deployment
state, SQL, connector config, Gateway rules, endpoints, or secrets.

## Entity and aspect mapping

| streamt fact | Entity | Aspects |
| --- | --- | --- |
| project plus catalog ID/fabric | `dataFlow` | `dataFlowInfo` |
| source or compiled topic output | `dataset` | `datasetProperties`; conditional `dataPlatformInstance` |
| actual compiled process | `dataJob` | `dataJobInfo`, `dataJobInputOutput` |
| process-free topic model | no `dataJob` | output Dataset only |
| Connect sink | `dataJob` | info and inputs; no output Dataset |
| exposure | omitted | none; warning |

### Exact streamt flow and job IDs

The one DataFlow uses orchestrator `streamt`, exact `--catalog-id` as flow ID,
and exact fabric as cluster. Every process DataJob is nested under that flow and
uses its exact neutral process logical name as job ID. Both encoded IDs have an
official maximum of 200 UTF-16 code units and MUST satisfy the construction and
parse/string parity above. An overlong process name fails with E508 rather than
truncating or hashing.

Process kind is not identity, so an engine change updates metadata instead of
creating a parallel job. The dependency-free renderer MUST produce bytes
identical to pinned SDK constructors. Final serialized URN collision checks are
mandatory because the official encoder escapes reserved characters while
literal percent sequences remain literal.

`dataFlowInfo` contains exact project name, optional non-blank description,
exact fabric as `env`, and required empty `customProperties`.
`dataJobInfo` contains exact logical name, optional description, exact fabric,
the flow URN, required empty `customProperties`, and process kind as the exact
union object:

```json
{
  "string": "flink"
}
```

The value is `flink`, `gateway`, or `connect`. Info aspects omit timestamps,
status, subtypes, external links, and every other optional field.

### Native Dataset URNs

Datasets never use hashes. Each Dataset URN is byte-identical to v1.7.0
`make_dataset_urn_with_platform_instance` for platform, exact physical
topic/alias, optional Kafka or required Gateway instance, and fabric.

Platform `kafka`, instance `main`, physical topic `orders`, fabric `PROD`
produces:

```text
urn:li:dataset:(urn:li:dataPlatform:kafka,main.orders,PROD)
```

Without an instance, `orders` is unprefixed. Oracle fixtures cover slash,
space, literal percent, comma, parentheses, U+241F, rejected U+001F, case,
Unicode, and final-URN collision detection. No lowercase-URN option exists;
operators must align DataHub Kafka ingestion casing.

`datasetProperties` contains exact logical name, optional non-blank
description, required empty legacy `tags`, and only the contract custom property
below. It omits qualified name, URI, timestamps, external links, schema,
columns, status, and subtypes.

```json
{
  "streamt.contract.status": "declared"
}
```

The value is `declared` or `enforced`; no contract means empty
`customProperties`. This is not a native DataContract, assertion, schema, or
proof of enforcement. No other custom property is allowed.

When an instance exists, `dataPlatformInstance.platform` is the exact
DataPlatform URN and `dataPlatformInstance.instance` is the exact typed
DataPlatformInstance URN. The latter uses the pinned typed
`DataPlatformInstanceUrn(DataPlatformUrn(...), instance)` behavior, including
reserved-character escaping; it does not use the inconsistent v1.7.0
`make_dataplatform_instance_urn` helper. Both values must agree with the Dataset
prefix.

With no Kafka instance, streamt deliberately omits `dataPlatformInstance`.
The v1.7.0 high-level Kafka Dataset helper emits a platform-only aspect in that
case, but the aspect is not required for Dataset identity. The first slice
claims exact Kafka Dataset-URN identity parity, not aspect-for-aspect parity
with that high-level helper. Official wrapper, file-source, and local closed
validation must accept the omission.

### Direct DataJob lineage

Every DataJob has one `dataJobInputOutput`. Input edges target exact direct
neutral dependency Datasets; output edges contain its output Dataset only when
one exists. A sink has empty output. The exporter never adds transitive,
process-to-process, exposure, inferred physical, or self-output inputs.

The exact v1 aspect contains all four arrays, including when empty:

- deprecated `inputDatasets`: `[]`;
- deprecated `outputDatasets`: `[]`;
- `inputDatasetEdges`: sorted minimal `{ "destinationUrn": "..." }` objects;
- `outputDatasetEdges`: the sorted minimal output edge or `[]`.

This must pass the SDK wrapper and file-source oracle. Failure blocks offline
export support; it does not authorize duplicating edges into deprecated arrays.
How a live server renders those edges remains explicitly unclaimed.

### Deferred owners, tags, sinks, and exposures

There is no owner/tag map. The exporter emits no `ownership`, `globalTags`,
corpuser, corpGroup, OwnershipType, or Tag object.

Each source/model declaration with an owner emits one
`W118_DATAHUB_OWNER_OMITTED`; a model gets one declaration warning, not one per
entity. Each declaration with tags emits one `W117_DATAHUB_TAGS_OMITTED`,
regardless of tag count. Messages do not echo owner or tag values.

Each sink emits `W115_DATAHUB_SINK_OUTPUT_OMITTED`; its DataJob and input edges
remain. Each exposure occurrence emits `W116_DATAHUB_EXPOSURE_OMITTED`, with
duplicates preserved. No destination or exposure placeholder is invented.

## Simplified DataHub MCP format and validation

The raw artifact is a top-level JSON array. Each member has exactly:

```json
{
  "aspect": {
    "json": {
      "customProperties": {},
      "name": "orders",
      "tags": []
    }
  },
  "aspectName": "datasetProperties",
  "changeType": "UPSERT",
  "entityType": "dataset",
  "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,main.orders,PROD)"
}
```

Every proposal has matching entity type/URN, no entity-key aspect,
`changeType: UPSERT`, one allowed aspect, and one `aspect.json`. Audit header,
headers, system metadata, timestamps, run IDs, and emitter metadata are absent.

| Entity type | Allowed aspects |
| --- | --- |
| `dataFlow` | `dataFlowInfo` |
| `dataset` | `datasetProperties`, `dataPlatformInstance` |
| `dataJob` | `dataJobInfo`, `dataJobInputOutput` |

The dependency-free validator enforces exact keys, types/defaults, URN grammar,
FabricType, cardinality, cross-references, and collisions. It fetches no schema
and imports no DataHub package.

Release tests only install exact `acryl-datahub==1.7.0`, reconstruct generated
aspects, require `MetadataChangeProposalWrapper.validate()`, compare
`.to_obj(simplified_structure=True)`, and prove official metadata-file semantic
round-trip. streamt canonical whitespace, not the official writer's whitespace,
is the wire contract.

All input, mapping, validation, collision, serialization, and file errors use
`E508_DATAHUB_INVALID`. Messages may name a safe option, logical declaration,
entity/aspect, or bounded JSON location, but never raw config, SDK exception,
endpoint, token, SQL, or connector configuration.

## Collision rules

Before output, reject two distinct logical declarations claiming one entity
URN, two declarations resolving to one Dataset URN, two processes resolving to
one DataJob URN, repeated `(entityUrn, aspectName)`, entity/URN mismatches,
oracle-inconsistent URNs, dangling dependencies/outputs, inconsistent
platform-instance aspects, and two different exact inputs that collide after
official URN encoding. Repeating an entity URN across its distinct allowed
aspects is required and is not a collision.

The complete topology validator also requires unique typed logical datasets
and processes; every dependency to resolve to exactly one emitted Dataset;
Flink and Gateway outputs to be the same-name Kafka or Gateway model Dataset,
respectively; Connect to have no output Dataset; and no output to belong to a
different model or multiple jobs. Every DataJob must nest the sole emitted
DataFlow, and its `dataJobInfo.flowUrn` must equal that flow. Edge sets must
contain no missing, extra, duplicate, transitive, or self input.

| Change | Identity effect |
| --- | --- |
| catalog ID | DataFlow and all DataJobs change; Datasets do not |
| fabric | every entity changes |
| physical topic/alias | that Dataset changes |
| platform ID/instance | that transport's Datasets change |
| process logical name | that DataJob changes |
| source logical name only | Dataset remains; properties change |
| description, owner, tags, contract, process kind | no URN changes |

Every raw identity change in this table is subject to the official percent-text
alias described above. For example, changing a catalog ID, platform/instance,
physical name, or process name from a reserved-character form to its literal
percent-encoded spelling can preserve the same URN.

Cross-file/remote collisions cannot be detected offline. Callers own catalog-ID
uniqueness and alignment with existing DataHub ingestion configuration.

## Deterministic ordering

Order is DataFlow `dataFlowInfo`; Datasets by UTF-8 URN with
`datasetProperties` then optional `dataPlatformInstance`; then DataJobs by URN
with `dataJobInfo` then `dataJobInputOutput`. Edges use UTF-8 destination-URN
order. Object keys sort by Unicode code point; arrays retain semantic order.
Warnings sort by safe logical location then code; duplicate exposures use their
projection ordinal.

Declaration/filesystem order, hash seed, locale, machine, checkout, and wheel
location MUST NOT affect output.

## Canonical output

Text mode without `--output-file` emits only canonical UTF-8 JSON: top-level
array, two-space indentation, `ensure_ascii=False`, sorted object keys, no
trailing spaces, LF, one final newline. Warnings use stderr in non-quiet text
mode. `--quiet` suppresses stdout and warning text. No banner or log contaminates
the array.

With global `--output json`, standard streamt envelope `data` is exactly:

```json
{
  "standard": "DataHub MCP",
  "release": "1.7.0",
  "api_version": "MetadataChangeProposal",
  "proposals": [],
  "counts": {
    "proposals": 0,
    "entities": {
      "dataFlow": 0,
      "dataset": 0,
      "dataJob": 0
    },
    "aspects": {
      "dataFlowInfo": 0,
      "datasetProperties": 0,
      "dataPlatformInstance": 0,
      "dataJobInfo": 0,
      "dataJobInputOutput": 0
    }
  },
  "output_file": null
}
```

`proposals` equals raw objects/order. Entity counts count unique URNs; aspect
counts count proposals; every shown key remains present. Successful exports
have DataFlow/dataFlowInfo count `1`. `output_file` is the exact display path
after a write, else null. JSON mode emits no warning text to stderr.

`--output-file` always writes canonical raw JSON. Text/file stdout is empty.
All bytes are built and validated before private same-directory staging,
write/flush/`fsync`, atomic replace, and cleanup. Every failure preserves an
existing destination and removes staging files.

## Warnings

| Code | Once per | Meaning |
| --- | --- | --- |
| `W115_DATAHUB_SINK_OUTPUT_OMITTED` | sink model | DataJob emitted; unknown destination Dataset omitted |
| `W116_DATAHUB_EXPOSURE_OMITTED` | exposure occurrence | no truthful v1 mapping |
| `W117_DATAHUB_TAGS_OMITTED` | declaration with tags | tag mapping deferred |
| `W118_DATAHUB_OWNER_OMITTED` | declaration with owner | ownership mapping deferred |

Warnings do not broaden the field allowlist.

## Privacy and repeatability

Forbidden in output, warnings, errors, and temporary names: DataHub
URLs/tokens/profiles; Kafka/Gateway endpoints or credentials; environment
variables; SQL/macros/paths; connector config/destinations; schemas/columns,
classifications, masks, policies, assertions, tests, freshness, SLAs, samples,
tag values, owner labels, exposure bodies; state/plans/operations/recovery; and
clocks, UUIDs, host/user/process/random data.

The explicit output path may appear in structured metadata. Catalog ID,
platform/instance/fabric, physical names, descriptions, contract state,
and topology are intentional sensitive metadata. Sentinel fixtures cover every
success/failure surface.

Identical inputs and streamt version MUST produce byte-identical raw output and
structured proposals across supported Python/platform/install contexts. There
is no partial result.

## Acceptance and release-oracle boundary

Before offline export support is claimed, tests prove all project shapes and
warning counts; direct-only inputs/existing outputs/empty sink outputs;
contract allowlist; every pinned encoded/complete URN bound; exact SDK URN
parity for reserved characters; Kafka no-instance/instance and required
Gateway platform/instance; every collision/dangling reference; local
validation and SDK wrapper/file parity; canonical bytes/counts on Python
3.10-3.12; zero runtime DataHub/network/state/provider/subprocess usage; no SDK
dependency in wheel/sdist; isolated installed-wheel behavior; secret
neutrality; and atomic file failures.

`acryl-datahub==1.7.0` is test-only. The exact offline CLI oracle is:

```text
DATAHUB_TELEMETRY_ENABLED=false datahub ingest run \
  --dry-run --strict-warnings --no-default-report \
  --no-spinner --no-progress -c <recipe>
```

The recipe uses a file source `config.path` and file sink `config.filename` in
one fresh temporary directory. Before importing DataHub, the oracle removes
all inherited `DATAHUB_*` variables, then sets
`DATAHUB_DATASET_URN_TO_LOWER=false` and `DATAHUB_TELEMETRY_ENABLED=false` and
asserts case preservation. This prevents the official dataset helper's
import-time lowercase setting from making parity environment-dependent.

Even in dry-run, the sink writes a local empty array on close; the gate promises
zero network and remote mutation, not zero temporary filesystem mutation. It
asserts source event/aspect counts rather than the final dry-run sink count.
CLI execution runs on Python 3.11; wrapper, reader, and canonical parity run on
Python 3.10-3.12 because the v1.7.0 CLI itself emits a Python-support warning
above 3.11.

The oracle reconstructs every proposal with
`MetadataChangeProposalWrapper.from_obj(copy.deepcopy(record))` before
validation because v1.7.0 mutates the supplied simplified object. It compares
the reconstructed simplified object to the untouched original.

`datahub ingest mcps <PATH>` is not offline: it loads client config and
publishes. A real-GMS ingestion/read-back gate is required before streamt may
claim server acceptance or live lineage behavior, but those claims are outside
this offline slice and do not block it.

## Deferred work

Owner/tag mapping and aspects; URL/token/publish/read/reconcile/state/delete;
native contracts/assertions; schemas/columns/field lineage/destinations/
exposures/domains/containers/status/subtypes/telemetry; multiple instances per
transport; DataPlatform bootstrap; real-GMS acceptance/live lineage behavior;
and Conduktor Console publication all require separate normative contracts.
